export function isAllowedPrivateUpdate(ctx, allowedUserIds, isGranted = () => false) {
    return ctx.chat?.type === "private"
        && typeof ctx.from?.id === "number"
        && ctx.chat.id === ctx.from.id
        && (allowedUserIds.includes(ctx.from.id) || isGranted(ctx.from.id));
}
export function createPrivateSender(send, allowedUserIds, isGranted = () => false) {
    return async (chatId, text) => {
        if (!allowedUserIds.includes(chatId) && !isGranted(chatId)) {
            throw new Error("unauthorized_telegram_destination");
        }
        await send(chatId, text);
    };
}
